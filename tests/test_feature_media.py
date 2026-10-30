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

"""Live photos, story music and audio listening reports (Layer 221-230)."""

import asyncio
import inspect
import os
import pickle
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from pyrogram import Client, enums, raw, types, utils
from pyrogram.errors import BroadcastPublicVotersForbidden, FloodWait, PeerIdInvalid, UserIdInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from tests.test_layer_roundtrip import offline_client, call, roundtrip, empty_updates, PEERS, RAW_CHANNEL


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
LIVE_VIDEO_ID = document_file_id(FileType.VIDEO, 3001)

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

RAW_LIVE_VIDEO = raw_document(3001, "video/mp4", [
    raw.types.DocumentAttributeVideo(duration=2.5, w=1080, h=1920),
])


def decoded(file_id):
    d = FileId.decode(file_id)
    return d.file_type, d.media_id, d.access_hash, d.file_reference


def assert_input_document(input_document, media_id, access_hash=55, file_reference=b"ref"):
    assert isinstance(input_document, raw.types.InputDocument)
    assert (input_document.id, input_document.access_hash, input_document.file_reference) == (
        media_id, access_hash, file_reference)


def sent(client, request_type):
    [request] = [q for q in client.sent if isinstance(q, request_type)]
    return request


# listen_to_audio -> messages.reportMusicListen (TDLib listenToAudio)

def test_listen_to_audio_reports_music_listen():
    client = offline_client(PEERS, lambda q: True)
    assert call(lambda: client.listen_to_audio(AUDIO_ID, 42)) is True

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.ReportMusicListen)
    assert request.listened_duration == 42
    assert_input_document(request.id, 1001)


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


# Received live photos: messageMediaPhoto.live_photo/video (TDLib messagePhoto.video, paidMediaPhoto.video)

def live_photo_media(**kwargs):
    # live_photo and video share flags.4: the server sends both or neither
    return raw.types.MessageMediaPhoto(photo=raw_photo(), live_photo=True, video=RAW_LIVE_VIDEO, **kwargs)


def assert_live_video(video, ttl_seconds=None):
    assert isinstance(video, types.Video)
    assert decoded(video.file_id) == (FileType.VIDEO, 3001, 55, b"ref")
    assert (video.width, video.height, video.duration, video.mime_type) == (1080, 1920, 2.5, "video/mp4")
    assert video.ttl_seconds == ttl_seconds


@pytest.mark.parametrize("ttl_seconds", [None, 10])
def test_live_photo_is_parsed(ttl_seconds):
    media = roundtrip(live_photo_media(ttl_seconds=ttl_seconds))
    assert media.live_photo is True

    photo = types.Photo._parse(None, media.photo, media.ttl_seconds, live_video=media.video)
    assert decoded(photo.file_id) == (FileType.PHOTO, 2001, 66, b"pref")
    assert (photo.width, photo.height, photo.ttl_seconds) == (800, 600, ttl_seconds)
    assert_live_video(photo.live_video, ttl_seconds)


def test_static_photo_has_no_live_video():
    media = roundtrip(raw.types.MessageMediaPhoto(photo=raw_photo()))
    assert not media.live_photo and media.video is None

    photo = types.Photo._parse(None, media.photo, media.ttl_seconds, live_video=media.video)
    assert photo.live_video is None
    assert types.Photo._parse(None, raw_photo()).live_video is None  # older callers


def test_live_photo_video_without_attributes():
    video = roundtrip(raw_document(3002, "video/mp4", []))
    live_video = types.Photo._parse(None, raw_photo(), live_video=video).live_video
    assert decoded(live_video.file_id)[:2] == (FileType.VIDEO, 3002)
    assert (live_video.width, live_video.height, live_video.duration) == (0, 0, 0)
    assert types.Photo._parse(None, raw_photo(), live_video=raw.types.DocumentEmpty(id=1)).live_video is None


def test_external_reply_to_live_photo():
    reply = roundtrip(raw.types.MessageReplyHeader(
        reply_to_msg_id=9, reply_to_peer_id=raw.types.PeerUser(user_id=7),
        reply_from=raw.types.MessageFwdHeader(date=1, from_name="Somebody"),
        reply_media=live_photo_media()
    ))
    info = asyncio.run(types.ExternalReplyInfo._parse(None, reply, {}, {}))
    assert info.media == enums.MessageMediaType.PHOTO
    assert_live_video(info.photo.live_video)


def test_paid_live_photo():
    media = roundtrip(raw.types.MessageMediaPaidMedia(stars_amount=5, extended_media=[
        raw.types.MessageExtendedMedia(media=live_photo_media()),
        raw.types.MessageExtendedMedia(media=raw.types.MessageMediaPhoto(photo=raw_photo(2002))),
    ]))
    live, static = types.PaidMedia._parse(None, media).extended_media
    assert_live_video(live.live_video)
    assert static.live_video is None and decoded(static.file_id)[1] == 2002


def test_bought_live_photo():
    client = offline_client(PEERS, lambda q: raw.types.payments.PaymentResult(updates=empty_updates(updates=[
        raw.types.UpdateMessageExtendedMedia(
            peer=raw.types.PeerChannel(channel_id=123), msg_id=9,
            extended_media=[raw.types.MessageExtendedMedia(media=live_photo_media())]
        )
    ])))

    async def get_payment_form(**kwargs):
        return SimpleNamespace(id=77)  # only the form id is used

    client.get_payment_form = get_payment_form
    [photo] = call(lambda: client.send_payment_form(chat_id=-100123, message_id=9))

    assert sent(client, raw.functions.payments.SendStarsForm).form_id == 77
    assert_live_video(photo.live_video)


def test_message_with_live_photo():
    from tests.test_layer_roundtrip import parse_message

    message = parse_message(raw.types.Message(
        id=1, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7),
        date=1, message="", media=live_photo_media()
    ), users=[RAW_USER])
    assert message.photo is not None
    assert_live_video(message.photo.live_video)


def live_photo_message():
    from tests.test_layer_roundtrip import parse_message

    return parse_message(raw.types.Message(
        id=1, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7),
        date=1, message="", media=live_photo_media()
    ), users=[RAW_USER])


def capture(message, method):
    calls = []

    async def fake(*args, **kwargs):
        calls.append((args, kwargs))

    setattr(message._client, method, fake)
    return calls


def test_message_bound_methods_pass_live_video():
    message = live_photo_message()
    photo_calls = capture(message, "send_photo")
    cached_calls = capture(message, "send_cached_media")

    call(lambda: message.reply_photo("photo.jpg", live_video="clip.mp4"))
    call(lambda: message.reply_cached_media(message.photo.file_id, live_video=message.photo.live_video.file_id))

    assert photo_calls[0][1]["live_video"] == "clip.mp4"
    assert cached_calls[0][1]["live_video"] == message.photo.live_video.file_id


def test_copy_keeps_live_photos():
    message = live_photo_message()
    calls = capture(message, "send_cached_media")

    call(lambda: message.copy("me"))

    [(args, kwargs)] = calls
    assert kwargs["file_id"] == message.photo.file_id
    assert kwargs["live_video"] == message.photo.live_video.file_id


def test_poll_media_live_photo():
    photo = asyncio.run(types.Poll._parse_media(None, roundtrip(live_photo_media())))
    assert_live_video(photo.live_video)


# Sending live photos: inputMediaPhoto/inputMediaUploadedPhoto.live_photo/video (TDLib inputPhoto.video)

def assert_live_photo_media(media, media_type):
    assert isinstance(media, media_type)
    assert media.live_photo is True
    assert_input_document(media.video, 3001)


def sent_message(media):
    # send_photo keeps sending until the answer contains the new message
    return empty_updates(updates=[raw.types.UpdateNewMessage(
        message=raw.types.Message(
            id=1, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7), date=1, message="",
            media=media
        ),
        pts=1, pts_count=1
    )], users=[RAW_USER])


def upload_respond(query):
    if isinstance(query, raw.functions.messages.UploadMedia):
        if isinstance(query.media, (raw.types.InputMediaUploadedDocument, raw.types.InputMediaDocumentExternal)):
            return raw.types.MessageMediaDocument(document=RAW_LIVE_VIDEO, video=True)
        return raw.types.MessageMediaPhoto(photo=raw_photo())
    return sent_message(live_photo_media())


def fake_uploads(client):
    saved = []

    async def save_file(path, *args, **kwargs):
        if path is None:
            return None  # like Client.save_file, e.g. for a missing thumbnail
        saved.append(path)
        return raw.types.InputFile(id=len(saved), parts=1, name=os.path.basename(path), md5_checksum="")

    client.save_file = save_file
    return saved


def test_send_photo_with_live_video_file_ids():
    client = offline_client(PEERS, upload_respond)
    message = call(lambda: client.send_photo(7, photo_file_id(2001), has_spoiler=True, live_video=LIVE_VIDEO_ID))

    [request] = client.sent  # nothing to upload
    assert isinstance(request, raw.functions.messages.SendMedia)
    assert_live_photo_media(request.media, raw.types.InputMediaPhoto)
    assert request.media.id.id == 2001 and request.media.spoiler
    assert message.id == 1 and message.photo is not None


def test_send_photo_uploads_live_video(tmp_path):
    photo, video = tmp_path / "photo.jpg", tmp_path / "live.mov"
    photo.write_bytes(b"jpeg")
    video.write_bytes(b"mov")
    client = offline_client(PEERS, upload_respond)
    saved = fake_uploads(client)
    call(lambda: client.send_photo(7, str(photo), live_video=str(video), business_connection_id="bc"))

    assert saved == [str(photo), str(video)]
    upload, invoke = client.sent
    assert isinstance(upload, raw.functions.messages.UploadMedia)
    assert upload.business_connection_id == "bc" and upload.peer.write() == PEERS[7].write()
    # TDLib's upload of the video of a live photo: an MPEG-4 video without size, kept as a video without sound too
    assert isinstance(upload.media, raw.types.InputMediaUploadedDocument)
    assert upload.media.file.name == "live.mov" and upload.media.mime_type == "video/mp4"
    assert upload.media.nosound_video and not upload.media.force_file
    [attribute] = upload.media.attributes
    assert isinstance(attribute, raw.types.DocumentAttributeVideo) and (attribute.w, attribute.h) == (0, 0)

    assert isinstance(invoke, raw.functions.InvokeWithBusinessConnection) and invoke.connection_id == "bc"
    assert isinstance(invoke.query, raw.functions.messages.SendMedia)
    assert_live_photo_media(invoke.query.media, raw.types.InputMediaUploadedPhoto)
    assert invoke.query.media.file.name == "photo.jpg"


def test_send_photo_by_url_with_live_video_url():
    client = offline_client(PEERS, upload_respond)
    call(lambda: client.send_photo(
        7, "https://example.org/photo.jpg", ttl_seconds=10, live_video="https://example.org/live.mp4"
    ))

    video_upload, photo_upload, request = client.sent
    assert isinstance(video_upload.media, raw.types.InputMediaDocumentExternal)
    assert video_upload.media.url == "https://example.org/live.mp4"
    # inputMediaPhotoExternal has no live photo fields: the photo is uploaded first too
    assert isinstance(photo_upload.media, raw.types.InputMediaPhotoExternal)
    assert photo_upload.media.url == "https://example.org/photo.jpg"
    assert_live_photo_media(request.media, raw.types.InputMediaPhoto)
    assert request.media.id.id == 2001 and request.media.ttl_seconds == 10


def test_send_photo_without_live_video():
    client = offline_client(PEERS, lambda q: sent_message(raw.types.MessageMediaPhoto(photo=raw_photo())))
    call(lambda: client.send_photo(7, photo_file_id(2001)))

    [request] = client.sent
    assert not request.media.live_photo and request.media.video is None


def test_live_video_must_be_a_video():
    client = offline_client(PEERS, upload_respond)
    with pytest.raises(ValueError):
        call(lambda: client.send_photo(7, photo_file_id(2001), live_video=AUDIO_ID))
    assert client.sent == []


def test_story_reply_photo_passes_live_video():
    def respond(query):
        if isinstance(query, raw.functions.users.GetUsers):
            return [roundtrip(RAW_USER)]
        return sent_message(live_photo_media())

    client = offline_client(PEERS, respond)
    story = parse_story(client, story_item())
    call(lambda: story.reply_photo(photo_file_id(2001), live_video=LIVE_VIDEO_ID))

    request = sent(client, raw.functions.messages.SendMedia)
    assert_live_photo_media(request.media, raw.types.InputMediaPhoto)
    assert isinstance(request.reply_to, raw.types.InputReplyToStory) and request.reply_to.story_id == 5


def test_send_media_group_with_live_photo():
    client = offline_client(PEERS, lambda q: empty_updates())
    call(lambda: client.send_media_group(7, [
        types.InputMediaPhoto(photo_file_id(2001), live_video=LIVE_VIDEO_ID),
        types.InputMediaPhoto(photo_file_id(2002)),
    ]))

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.SendMultiMedia)
    live, static = (m.media for m in request.multi_media)
    assert_live_photo_media(live, raw.types.InputMediaPhoto)
    assert live.id.id == 2001
    assert static.id.id == 2002 and not static.live_photo and static.video is None


def test_send_paid_media_with_live_photo():
    client = offline_client(PEERS, lambda q: empty_updates())
    call(lambda: client.send_paid_media(-100123, 5, [
        types.InputMediaPhoto(photo_file_id(2001), live_video=LIVE_VIDEO_ID),
    ]))

    [request] = client.sent
    assert isinstance(request.media, raw.types.InputMediaPaidMedia) and request.media.stars_amount == 5
    [media] = request.media.extended_media
    assert_live_photo_media(media, raw.types.InputMediaPhoto)


def test_edit_message_media_with_live_photo():
    client = offline_client(PEERS, lambda q: empty_updates())
    call(lambda: client.edit_message_media(
        7, 9, types.InputMediaPhoto(photo_file_id(2001), caption="live", live_video=LIVE_VIDEO_ID)
    ))

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.EditMessage) and request.id == 9
    assert_live_photo_media(request.media, raw.types.InputMediaPhoto)
    assert request.message == "live"


def inline_client(respond):
    client = offline_client(PEERS, respond)

    async def dc_id():
        return 2  # the DC of inline_message_id(): edit_inline_media then invokes through the client itself

    client.storage.dc_id = dc_id
    return client


def inline_message_id():
    return utils.pack_inline_message_id(raw.types.InputBotInlineMessageID(dc_id=2, id=5, access_hash=6))


def test_edit_inline_media_with_live_photo_file_ids():
    client = inline_client(lambda q: True)
    assert call(lambda: client.edit_inline_media(
        inline_message_id(), types.InputMediaPhoto(photo_file_id(2001), live_video=LIVE_VIDEO_ID)
    )) is True

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.EditInlineBotMessage) and request.id.id == 5
    assert_live_photo_media(request.media, raw.types.InputMediaPhoto)
    assert request.media.id.id == 2001


def test_edit_inline_media_uploads_live_photo(tmp_path):
    photo, video = tmp_path / "photo.jpg", tmp_path / "live.mp4"
    photo.write_bytes(b"jpeg")
    video.write_bytes(b"mp4")
    client = inline_client(lambda q: upload_respond(q) if isinstance(q, raw.functions.messages.UploadMedia) else True)
    saved = fake_uploads(client)
    call(lambda: client.edit_inline_media(
        inline_message_id(), types.InputMediaPhoto(str(photo), has_spoiler=True, live_video=str(video))
    ))

    assert saved == [str(photo), str(video)]
    video_upload, photo_upload, request = client.sent
    assert isinstance(video_upload.peer, raw.types.InputPeerSelf)
    assert isinstance(video_upload.media, raw.types.InputMediaUploadedDocument)
    assert isinstance(photo_upload.peer, raw.types.InputPeerSelf)
    assert_live_photo_media(photo_upload.media, raw.types.InputMediaUploadedPhoto)
    # the uploaded photo is sent as a photo, keeping its spoiler and live video
    assert_live_photo_media(request.media, raw.types.InputMediaPhoto)
    assert request.media.id.id == 2001 and request.media.spoiler


def test_edit_inline_media_uploads_static_photo(tmp_path):
    photo = tmp_path / "photo.jpg"
    photo.write_bytes(b"jpeg")
    client = inline_client(lambda q: upload_respond(q) if isinstance(q, raw.functions.messages.UploadMedia) else True)
    fake_uploads(client)
    call(lambda: client.edit_inline_media(inline_message_id(), types.InputMediaPhoto(str(photo), has_spoiler=True)))

    upload, request = client.sent  # used to be sent as a document (AttributeError)
    assert isinstance(upload.media, raw.types.InputMediaUploadedPhoto) and not upload.media.live_photo
    assert isinstance(request.media, raw.types.InputMediaPhoto) and request.media.spoiler
    assert not request.media.live_photo and request.media.video is None


def test_edit_inline_media_uploads_video_with_spoiler(tmp_path):
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"mp4")
    client = inline_client(lambda q: upload_respond(q) if isinstance(q, raw.functions.messages.UploadMedia) else True)
    fake_uploads(client)
    call(lambda: client.edit_inline_media(inline_message_id(), types.InputMediaVideo(str(video), has_spoiler=True)))

    upload, request = client.sent
    assert isinstance(upload.media, raw.types.InputMediaUploadedDocument) and upload.media.spoiler
    # the uploaded video keeps its spoiler (it used to be dropped)
    assert isinstance(request.media, raw.types.InputMediaDocument) and request.media.spoiler
    assert request.media.id.id == 3001


def test_send_poll_media_with_live_photos():
    client = offline_client(PEERS, lambda q: empty_updates())
    call(lambda: client.send_poll(
        -100123, "Q?",
        [types.PollOption("a", media=types.InputMediaPhoto(photo_file_id(2002), live_video=LIVE_VIDEO_ID)),
         types.PollOption("b")],
        media=types.InputMediaPhoto(photo_file_id(2001), live_video=LIVE_VIDEO_ID)
    ))

    [request] = client.sent
    assert_live_photo_media(request.media.attached_media, raw.types.InputMediaPhoto)
    first, second = request.media.poll.answers
    assert_live_photo_media(first.media, raw.types.InputMediaPhoto)
    assert first.media.id.id == 2002 and second.media is None


def test_poll_media_uploads_live_video(tmp_path):
    video = tmp_path / "live.mp4"
    video.write_bytes(b"mp4")
    client = offline_client(PEERS, upload_respond)
    saved = fake_uploads(client)
    media = call(lambda: utils.get_input_poll_media(
        client, PEERS[-100123], types.InputMediaPhoto("https://example.org/photo.jpg", live_video=str(video))
    ))

    assert saved == [str(video)]
    video_upload, photo_upload = client.sent
    assert video_upload.peer.write() == PEERS[-100123].write()
    assert isinstance(video_upload.media, raw.types.InputMediaUploadedDocument)
    assert isinstance(photo_upload.media, raw.types.InputMediaPhotoExternal)
    assert_live_photo_media(media, raw.types.InputMediaPhoto)
    assert media.id.id == 2001


def test_ephemeral_and_welcome_media_keep_live_video():
    client = offline_client(PEERS, upload_respond)
    content = call(lambda: utils.get_input_message_content(
        client, -100123, media=types.InputMediaPhoto(photo_file_id(2001), caption="live", live_video=LIVE_VIDEO_ID)
    ))

    assert client.sent == []  # file ids: nothing to upload
    assert_live_photo_media(content["media"], raw.types.InputMediaPhoto)
    assert content["media"].id.id == 2001 and content["message"] == "live"


def test_send_cached_media_with_live_video():
    client = offline_client(PEERS, upload_respond)
    message = call(lambda: client.send_cached_media(7, photo_file_id(2001), live_video=LIVE_VIDEO_ID))

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.SendMedia)
    assert_live_photo_media(request.media, raw.types.InputMediaPhoto)
    assert request.media.id.id == 2001 and message.id == 1


def test_send_cached_media_live_video_needs_a_photo():
    client = offline_client(PEERS, upload_respond)
    with pytest.raises(ValueError):
        call(lambda: client.send_cached_media(7, AUDIO_ID, live_video=LIVE_VIDEO_ID))
    assert client.sent == []


def test_copy_media_group_keeps_live_photos():
    client = offline_client(PEERS, lambda q: empty_updates())

    async def get_media_group(chat_id, message_id):
        live = types.Photo._parse(None, roundtrip(raw_photo(2001)), live_video=roundtrip(RAW_LIVE_VIDEO))
        static = types.Photo._parse(None, roundtrip(raw_photo(2002)))
        return [
            types.Message(id=1, media=enums.MessageMediaType.PHOTO, caption="live", photo=live),
            types.Message(id=2, media=enums.MessageMediaType.PHOTO, photo=static),
        ]

    client.get_media_group = get_media_group
    call(lambda: client.copy_media_group(7, -100123, 1))

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.SendMultiMedia)
    live, static = (m.media for m in request.multi_media)
    assert_live_photo_media(live, raw.types.InputMediaPhoto)
    assert live.id.id == 2001
    assert static.id.id == 2002 and not static.live_photo and static.video is None


def parse_message(client, message_id, media, text, entities, above):
    return types.Message._parse(
        client,
        roundtrip(raw.types.Message(
            id=message_id, peer_id=raw.types.PeerUser(user_id=7), date=1, message=text, entities=entities,
            invert_media=above, media=media
        )),
        {7: roundtrip(RAW_USER)}, {}, replies=0
    )


def photo_message(client, message_id, caption, entities, above):
    return parse_message(
        client, message_id, raw.types.MessageMediaPhoto(photo=raw_photo(2000 + message_id)), caption, entities, above
    )


async def album_messages(client, album, above=()):
    return [
        await photo_message(client, i, caption, entities, i in above)
        for i, (caption, entities) in enumerate(album, start=1)
    ]


def client_with_mention_error(mention_error):
    """A client that doesn't know user 7 if asking Telegram about them gives this error."""
    def respond(query):
        if isinstance(query, raw.functions.users.GetUsers):
            raise mention_error
        return empty_updates()

    return offline_client({k: v for k, v in PEERS.items() if k != 7} if mention_error else PEERS, respond)


def copy_album(*album, above=(), mention_error=None, **kwargs):
    """Copy an album of (caption, raw entities) photos and return the request that was sent."""
    client = client_with_mention_error(mention_error)

    async def get_media_group(chat_id, message_id):
        return await album_messages(client, album, above)

    client.get_media_group = get_media_group
    call(lambda: client.copy_media_group(-100123, -100123, 1, **kwargs))

    return sent(client, raw.functions.messages.SendMultiMedia)


def written(entities):
    return [entity.write() for entity in entities]


ENTITY_FIELDS = {
    enums.MessageEntityType.PRE: {"language": "python"},
    enums.MessageEntityType.BLOCKQUOTE: {"collapsed": True},
    enums.MessageEntityType.TEXT_LINK: {"url": "https://example.com"},
    enums.MessageEntityType.CUSTOM_EMOJI: {"document_id": 42},
    enums.MessageEntityType.FORMATTED_DATE: {"date": 1, "relative": True},
    enums.MessageEntityType.DIFF_REPLACE: {"old_text": "old"},
}


@pytest.mark.parametrize(
    "entity_type", [t for t in enums.MessageEntityType if t is not enums.MessageEntityType.TEXT_MENTION]
)
def test_copy_media_group_keeps_every_kind_of_formatting(entity_type):
    entity = entity_type.value(offset=0, length=7, **ENTITY_FIELDS.get(entity_type, {}))

    [media] = copy_album(("caption", [entity])).multi_media

    assert written(media.entities) == [entity.write()]


# The emoji is two UTF-16 code units, so these offsets are not the string indices
FORMATTED_CAPTION = "😀 bold link"
FORMATTED_CAPTION_ENTITIES = [
    raw.types.MessageEntityBold(offset=3, length=4),
    raw.types.MessageEntityTextUrl(offset=8, length=4, url="https://example.com"),
]


def test_copy_media_group_keeps_entity_offsets():
    [media] = copy_album((FORMATTED_CAPTION, FORMATTED_CAPTION_ENTITIES)).multi_media

    assert media.message == FORMATTED_CAPTION
    assert written(media.entities) == written(FORMATTED_CAPTION_ENTITIES)


def test_copy_media_group_keeps_text_mentions():
    [media] = copy_album(("user", [raw.types.MessageEntityMentionName(offset=0, length=4, user_id=7)])).multi_media

    [mention] = media.entities
    assert isinstance(mention, raw.types.InputMessageEntityMentionName)
    assert (mention.offset, mention.length) == (0, 4)
    assert mention.user_id.write() == PEERS[7].write()


@pytest.mark.parametrize("user_id, error", [
    (7, PeerIdInvalid),
    (7, UserIdInvalid),
    (8, None),  # A user who is missing from the response
])
def test_copy_media_group_leaves_out_mentions_it_cannot_reach(user_id, error):
    bold = raw.types.MessageEntityBold(offset=0, length=4)
    mention = raw.types.MessageEntityMentionName(offset=0, length=4, user_id=user_id)

    [media] = copy_album(("user", [bold, mention]), mention_error=error).multi_media

    assert media.message == "user" and written(media.entities) == [bold.write()]


@pytest.mark.parametrize("user_ids", [[1000] * 100, list(range(1000, 1100))], ids=["one user", "different users"])
def test_many_mentions_of_unknown_users_are_asked_for_in_one_request(user_ids):
    """Telegram only knows the even ones."""
    def respond(query):
        if isinstance(query, raw.functions.users.GetUsers):
            return [raw.types.User(id=i.user_id, access_hash=1, first_name="m") if i.user_id % 2 == 0
                    else raw.types.UserEmpty(id=i.user_id) for i in query.id]
        return empty_updates()

    client = offline_client(PEERS, respond)

    async def fetch_peers(found):
        client.resolve_peer = client.storage.get_peer_by_id = known_after_fetching(client, found)

    client.fetch_peers = fetch_peers
    entities = [raw.types.MessageEntityMentionName(offset=i, length=1, user_id=user_id) for i, user_id in enumerate(user_ids)]
    users = {user_id: roundtrip(raw.types.User(id=user_id, access_hash=1, first_name="m", min=True)) for user_id in user_ids}
    message = asyncio.run(types.Message._parse(client, roundtrip(raw.types.Message(
        id=1, peer_id=raw.types.PeerUser(user_id=7), date=1, message="x" * len(user_ids), media=raw.types.MessageMediaPhoto(photo=raw_photo()),
        entities=entities
    )), {**users, 7: roundtrip(RAW_USER)}, {}, replies=0))

    call(lambda: message.copy(-100123))

    get_users = [q for q in client.sent if isinstance(q, raw.functions.users.GetUsers)]
    send_media = sent(client, raw.functions.messages.SendMedia)
    assert len(get_users) == 1 and sorted(i.user_id for i in get_users[0].id) == sorted(set(user_ids))
    assert [e.offset for e in send_media.entities or []] == [i for i, user_id in enumerate(user_ids) if user_id % 2 == 0]


def known_after_fetching(client, found):
    known = {user.id: raw.types.InputPeerUser(user_id=user.id, access_hash=1) for user in found if isinstance(user, raw.types.User)}

    async def get_peer_by_id(peer_id):
        return {**PEERS, **known}[peer_id]

    return get_peer_by_id


def test_copy_media_group_does_not_hide_other_errors_of_a_mention():
    mention = raw.types.MessageEntityMentionName(offset=0, length=4, user_id=7)

    with pytest.raises(FloodWait):
        copy_album(("user", [mention]), mention_error=FloodWait)


PLAIN_CAPTIONS = [
    "",
    "None",
    "**not bold** <b>not html</b>",
    "[hidden link](https://example.com)",
    '<a href="https://example.com">hidden link</a>',
    '<a href="tg://user?id=999">mention</a>',
    '<emoji id="42">😀</emoji>',
]


@pytest.mark.parametrize("caption", PLAIN_CAPTIONS)
@pytest.mark.parametrize("parse_mode", [None, enums.ParseMode.HTML])
def test_copy_media_group_does_not_parse_original_captions(caption, parse_mode):
    [media] = copy_album((caption, None), parse_mode=parse_mode).multi_media

    assert media.message == caption and not media.entities


@pytest.mark.parametrize("captions, expected", [
    (["**new**", None, ""], ["new", "original", ""]),
    (["**new**"], ["new", "original", "original"]),
    ("**new**", ["new", "", ""]),
    ("", ["", "", ""]),
])
def test_copy_media_group_new_captions_replace_the_original_ones(captions, expected):
    italic = [raw.types.MessageEntityItalic(offset=0, length=8)]
    bold = [raw.types.MessageEntityBold(offset=0, length=3)]
    formatting = {"new": written(bold), "original": written(italic), "": []}

    multi_media = copy_album(*[("original", italic)] * 3, captions=captions).multi_media

    assert [(m.message, written(m.entities)) for m in multi_media] == [(e, formatting[e]) for e in expected]


def test_copy_media_group_keeps_caption_position():
    album = [("first", None), ("second", None)]

    assert not copy_album(*album).invert_media
    assert copy_album(*album, above=[2]).invert_media
    assert copy_album(*album, invert_media=True).invert_media
    assert not copy_album(*album, above=[1, 2], invert_media=False).invert_media


NEW_CAPTION = "<b>new</b> **x**"
PARSED_NEW_CAPTIONS = [
    (enums.ParseMode.DISABLED, "<b>new</b> **x**", []),
    (enums.ParseMode.HTML, "new **x**", [raw.types.MessageEntityBold(offset=0, length=3)]),
    (enums.ParseMode.MARKDOWN, "<b>new</b> x", [raw.types.MessageEntityBold(offset=11, length=1)]),
]


@pytest.mark.parametrize("captions", [NEW_CAPTION, [NEW_CAPTION]])
@pytest.mark.parametrize("parse_mode, message, entities", PARSED_NEW_CAPTIONS)
def test_copy_media_group_new_captions_follow_parse_mode(captions, parse_mode, message, entities):
    [media] = copy_album(("original", None), captions=captions, parse_mode=parse_mode).multi_media

    assert media.message == message and written(media.entities) == written(entities)


@pytest.fixture(params=["Message.copy", "Client.copy_message"])
def copy_photo(request):
    """Copy a photo with the given original caption and return the request that was sent."""
    def copy(original, entities=None, above=False, markup=None, mention_error=None, **kwargs):
        client = client_with_mention_error(mention_error)
        message = asyncio.run(photo_message(client, 1, original, entities, above))
        message.reply_markup = markup

        async def get_messages(chat_id, message_id, replies):
            return message

        client.get_messages = get_messages

        if request.param == "Message.copy":
            call(lambda: message.copy(-100123, **kwargs))
        else:
            call(lambda: client.copy_message(-100123, -100123, 1, **kwargs))

        return sent(client, raw.functions.messages.SendMedia)

    return copy


@pytest.mark.parametrize("caption", PLAIN_CAPTIONS)
@pytest.mark.parametrize("parse_mode", [None, enums.ParseMode.HTML])
def test_copy_does_not_parse_original_captions(copy_photo, caption, parse_mode):
    request = copy_photo(caption, parse_mode=parse_mode)

    assert request.message == caption and not request.entities


def test_copy_keeps_caption_formatting(copy_photo):
    request = copy_photo(FORMATTED_CAPTION, FORMATTED_CAPTION_ENTITIES)

    assert request.message == FORMATTED_CAPTION
    assert written(request.entities) == written(FORMATTED_CAPTION_ENTITIES)


@pytest.mark.parametrize("parse_mode, message, entities", PARSED_NEW_CAPTIONS)
def test_copy_new_captions_follow_parse_mode(copy_photo, parse_mode, message, entities):
    request = copy_photo("original", caption=NEW_CAPTION, parse_mode=parse_mode)

    assert request.message == message and written(request.entities) == written(entities)


def test_copy_empty_caption_removes_the_original_one(copy_photo):
    assert copy_photo("original", caption="").message == ""


def test_copy_keeps_caption_position(copy_photo):
    assert not copy_photo("caption").invert_media
    assert copy_photo("caption", above=True).invert_media
    assert copy_photo("caption", invert_media=True).invert_media
    assert not copy_photo("caption", above=True, invert_media=False).invert_media


@pytest.mark.parametrize("user_id, error", [
    (7, PeerIdInvalid),
    (7, UserIdInvalid),
    (8, None),  # A user who is missing from the response
])
def test_copy_leaves_out_mentions_it_cannot_reach(copy_photo, user_id, error):
    bold = raw.types.MessageEntityBold(offset=0, length=4)
    mention = raw.types.MessageEntityMentionName(offset=0, length=4, user_id=user_id)

    request = copy_photo("user", [bold, mention], mention_error=error)

    assert request.message == "user" and written(request.entities) == [bold.write()]


KEYBOARD = types.InlineKeyboardMarkup([[types.InlineKeyboardButton("button", callback_data=b"data")]])


def test_copy_message_only_sends_the_reply_markup_it_is_given():
    client = offline_client(PEERS, lambda q: empty_updates())
    message = asyncio.run(photo_message(client, 1, "caption", None, False))
    message.reply_markup = KEYBOARD

    async def get_messages(chat_id, message_id, replies):
        return message

    client.get_messages = get_messages

    call(lambda: client.copy_message(-100123, -100123, 1))
    call(lambda: client.copy_message(-100123, -100123, 1, reply_markup=KEYBOARD))

    default, explicit = client.sent
    assert not default.reply_markup and explicit.reply_markup


def copy_media(media, text="", entities=None, above=False, markup=None, **kwargs):
    """Copy a message with Message.copy and return the request that was sent."""
    client = offline_client(PEERS, lambda q: empty_updates())

    async def is_bot(value=None):
        return True

    client.storage.is_bot = is_bot
    message = asyncio.run(parse_message(client, 1, media, text, entities, above))
    message.reply_markup = markup

    call(lambda: message.copy(-100123, **kwargs))

    [request] = client.sent
    return request


def raw_poll(question, *answers, question_entities=None, results=None, **settings):
    return raw.types.MessageMediaPoll(
        poll=raw.types.Poll(
            id=1, hash=0,
            question=raw.types.TextWithEntities(text=question, entities=question_entities or []),
            answers=[
                raw.types.PollAnswer(text=raw.types.TextWithEntities(text=text, entities=entities or []), option=bytes([i]))
                for i, (text, entities) in enumerate(answers)
            ],
            **settings
        ),
        results=results or raw.types.PollResults()
    )


def test_copy_does_not_parse_the_text_of_polls():
    bold = raw.types.MessageEntityBold(offset=0, length=3)

    poll = copy_media(raw_poll("[hidden](https://example.com) **q**", ("<b>one</b>", None), ("two", [bold]))).media.poll

    assert poll.question.text == "[hidden](https://example.com) **q**" and not poll.question.entities
    assert [answer.text.text for answer in poll.answers] == ["<b>one</b>", "two"]
    assert not poll.answers[0].text.entities and written(poll.answers[1].text.entities) == [bold.write()]


def test_copy_keeps_the_formatting_of_poll_questions():
    bold = raw.types.MessageEntityBold(offset=0, length=1)

    poll = copy_media(raw_poll("q", ("one", None), ("two", None), question_entities=[bold])).media.poll

    assert poll.question.text == "q" and written(poll.question.entities) == [bold.write()]


GEO_POINT = raw.types.GeoPoint(long=1.0, lat=2.0, access_hash=3)
MEDIA_WITHOUT_CAPTION = {
    "contact": raw.types.MessageMediaContact(phone_number="1", first_name="a", last_name="", vcard="", user_id=0),
    "location": raw.types.MessageMediaGeo(geo=GEO_POINT),
    "venue": raw.types.MessageMediaVenue(
        geo=GEO_POINT, title="t", address="a", provider="foursquare", venue_id="id", venue_type="food"
    ),
    "poll": raw_poll("q", ("one", None), ("two", None)),
}


@pytest.mark.parametrize("media", MEDIA_WITHOUT_CAPTION.values(), ids=MEDIA_WITHOUT_CAPTION.keys())
def test_copy_passes_the_options_of_media_without_caption(media):
    request = copy_media(
        media, protect_content=True, disable_notification=True, reply_markup=KEYBOARD, reply_to_message_id=3,
        quote_text="<b>q</b>", parse_mode=enums.ParseMode.HTML
    )

    assert request.noforwards and request.silent and request.reply_markup
    assert request.reply_to.reply_to_msg_id == 3 and request.reply_to.quote_text == "q"
    assert written(request.reply_to.quote_entities) == [raw.types.MessageEntityBold(offset=0, length=1).write()]


def test_copy_passes_the_options_of_games():
    game = raw.types.Game(id=1, access_hash=2, short_name="game", title="t", description="d", photo=raw_photo())

    request = copy_media(
        raw.types.MessageMediaGame(game=game), protect_content=True, disable_notification=True, reply_markup=KEYBOARD,
        reply_to_message_id=3, reply_to_chat_id=7, quote_text="<b>q</b>", parse_mode=enums.ParseMode.HTML,
        schedule_date=datetime(2030, 1, 1, tzinfo=timezone.utc)
    )

    assert request.noforwards and request.silent and request.reply_markup and request.reply_to.reply_to_msg_id == 3
    assert request.reply_to.reply_to_peer_id.write() == PEERS[7].write()
    assert request.reply_to.quote_text == "q" and request.schedule_date == 1893456000


def test_copy_of_a_venue_without_an_id():
    request = copy_media(raw.types.MessageMediaVenue(
        geo=GEO_POINT, title="t", address="a", provider="", venue_id="", venue_type=""
    ))

    assert (request.media.venue_id, request.media.venue_type) == ("", "")


def test_copy_keeps_the_spoiler():
    photo = raw.types.MessageMediaPhoto(photo=raw_photo())
    covered = raw.types.MessageMediaPhoto(photo=raw_photo(), spoiler=True)

    assert copy_media(covered).media.spoiler
    assert copy_media(photo, has_spoiler=True).media.spoiler
    assert not copy_media(covered, has_spoiler=False).media.spoiler
    assert not copy_media(photo).media.spoiler


def test_copy_passes_the_quote_of_media():
    photo = raw.types.MessageMediaPhoto(photo=raw_photo())
    bold = [raw.types.MessageEntityBold(offset=0, length=1)]
    quote = {"reply_to_message_id": 3, "quote_text": "<b>q</b>", "parse_mode": enums.ParseMode.HTML}

    # The original caption isn't parsed, but the quote is
    request = copy_media(photo, "<b>original</b>", **quote)
    assert request.message == "<b>original</b>" and not request.entities
    assert request.reply_to.quote_text == "q" and written(request.reply_to.quote_entities) == written(bold)

    request = copy_media(photo, "original", caption="<b>new</b>", **quote)
    assert request.message == "new" and written(request.entities) == [raw.types.MessageEntityBold(offset=0, length=3).write()]
    assert request.reply_to.quote_text == "q" and written(request.reply_to.quote_entities) == written(bold)


def test_copy_of_a_text_leaves_out_mentions_it_cannot_reach():
    bold = raw.types.MessageEntityBold(offset=0, length=4)
    mention = raw.types.MessageEntityMentionName(offset=0, length=4, user_id=8)

    request = copy_media(None, "user", [bold, mention])

    assert request.message == "user" and written(request.entities) == [bold.write()]


@pytest.mark.parametrize("media, text", [
    (None, "text"),
    (raw.types.MessageMediaPhoto(photo=raw_photo()), "caption"),
    (raw.types.MessageMediaGeo(geo=raw.types.GeoPoint(long=1.0, lat=2.0, access_hash=3)), ""),
], ids=["text", "photo", "location"])
def test_copy_keeps_the_reply_markup_unless_told_otherwise(media, text):
    assert copy_media(media, text, markup=KEYBOARD).reply_markup
    assert not copy_media(media, text, markup=KEYBOARD, reply_markup=None).reply_markup


def test_copy_of_a_link_preview_without_text():
    webpage = raw.types.WebPage(id=1, url="https://example.com", display_url="example.com", hash=0)

    request = copy_media(
        raw.types.MessageMediaWebPage(webpage=webpage), above=True, protect_content=True, reply_to_message_id=3,
        quote_text="<b>q</b>", parse_mode=enums.ParseMode.HTML
    )

    assert isinstance(request.media, raw.types.InputMediaWebPage) and request.media.url == "https://example.com"
    assert request.invert_media and request.noforwards and request.reply_to.quote_text == "q"


def test_copy_keeps_the_position_of_the_link_preview():
    preview = raw.types.MessageMediaWebPage(webpage=raw.types.WebPageEmpty(id=1))
    url = "https://example.com"

    assert not copy_media(preview, url).invert_media
    assert copy_media(preview, url, above=True).invert_media
    assert copy_media(preview, url, invert_media=True).invert_media
    assert not copy_media(preview, url, above=True, invert_media=False).invert_media


@pytest.mark.parametrize("quote, parse_mode, text, entities", [
    ("<b>q</b>", enums.ParseMode.HTML, "q", [raw.types.MessageEntityBold(offset=0, length=1)]),
    ("**q**", None, "q", [raw.types.MessageEntityBold(offset=0, length=1)]),
    ("**q**", enums.ParseMode.DISABLED, "**q**", []),
    ("q", None, "q", []),
])
def test_copy_media_group_parses_the_quote_once(quote, parse_mode, text, entities):
    reply_to = copy_album(("a", None), reply_to_message_id=3, quote_text=quote, parse_mode=parse_mode).reply_to

    assert reply_to.quote_text == text and written(reply_to.quote_entities) == written(entities)


def test_copy_media_group_replies_to_stories():
    reply_to = copy_album(("a", None), reply_to_story_id=9).reply_to

    assert isinstance(reply_to, raw.types.InputReplyToStory) and reply_to.story_id == 9
    assert reply_to.peer.write() == PEERS[-100123].write()


def test_add_task_to_todo_returns_the_message():
    client = offline_client(PEERS, lambda q: empty_updates(updates=[raw.types.UpdateBotNewBusinessMessage(
        connection_id="connection", qts=1,
        message=raw.types.Message(id=1, peer_id=raw.types.PeerUser(user_id=7), date=1, message="")
    )], users=[RAW_USER]))

    async def get_messages(chat_id, message_id):
        return types.Message(id=1, todo=SimpleNamespace(tasks=[]))

    client.get_messages = get_messages

    message = call(lambda: client.add_task_to_todo(7, 1, []))

    assert isinstance(message, types.Message) and message.id == 1
    assert message.business_connection_id == "connection"


def pickled_album(client, *album):
    """The messages of an album after crossing processes, which leaves them without a client."""
    return pickle.loads(pickle.dumps(asyncio.run(album_messages(client, album))))


def sent_photos(count, reply_to, chat=RAW_CHANNEL):
    return empty_updates(updates=[raw.types.UpdateNewChannelMessage(
        message=raw.types.Message(
            id=50 + i, peer_id=raw.types.PeerChannel(channel_id=123), date=1, message="sent", reply_to=reply_to,
            media=raw.types.MessageMediaPhoto(photo=raw_photo(3000 + i))
        ),
        pts=1, pts_count=1
    ) for i in range(count)], chats=[chat])


@pytest.mark.parametrize("method", ["copy_media_group", "forward_media_group"])
@pytest.mark.parametrize("reply_to", [
    raw.types.MessageReplyHeader(reply_to_msg_id=7, forum_topic=True),
    raw.types.MessageReplyHeader(reply_to_msg_id=3),
], ids=["topic", "reply"])
def test_media_group_results_are_parsed_without_reading(method, reply_to):
    client = offline_client(PEERS, lambda q: sent_photos(2, reply_to))
    album = pickled_album(client, ("a", None), ("b", None))

    messages = call(lambda: getattr(client, method)(-100123, -100123, 1, media_group=album))

    [request] = client.sent
    assert isinstance(request, (raw.functions.messages.SendMultiMedia, raw.functions.messages.ForwardMessages))
    assert [message.id for message in messages] == [50, 51]
    assert messages[0].caption == "sent" and messages[0].link and callable(messages[0].edit_caption)
    assert [message.id for message in pickle.loads(pickle.dumps(list(messages)))] == [50, 51]


def test_media_group_new_parameters_come_last():
    assert list(inspect.signature(Client.copy_media_group).parameters)[-2:] == ["media_group", "sleep_threshold"]
    assert list(inspect.signature(Client.forward_media_group).parameters)[-1] == "media_group"


def test_forward_media_group_uses_the_given_messages():
    client = offline_client(PEERS, lambda q: empty_updates())
    album = pickled_album(client, ("a", None), ("b", None))

    call(lambda: client.forward_media_group(-100123, -100123, 1, media_group=album))

    [request] = client.sent
    assert isinstance(request, raw.functions.messages.ForwardMessages) and request.id == [1, 2]


def test_copy_media_group_accepts_messages_without_a_client():
    bold = raw.types.MessageEntityBold(offset=0, length=4)
    mention = raw.types.MessageEntityMentionName(offset=0, length=4, user_id=7)
    client = offline_client(PEERS, lambda q: empty_updates())
    album = pickled_album(client, ("user", [bold, mention]))

    call(lambda: client.copy_media_group(-100123, -100123, 1, media_group=album))

    [request] = client.sent
    [media] = request.multi_media
    assert media.message == "user" and written(media.entities[:1]) == [bold.write()]
    assert isinstance(media.entities[1], raw.types.InputMessageEntityMentionName)


@pytest.mark.parametrize("kwargs, expected", [
    ({}, 60),
    ({"sleep_threshold": None}, None),
    ({"sleep_threshold": 5}, 5),
])
def test_copy_media_group_sleep_threshold(kwargs, expected):
    client = offline_client(PEERS, lambda q: empty_updates())
    album = pickled_album(client, ("a", None))
    invoke = client.invoke
    thresholds = []

    async def recording_invoke(query, *args, **invoke_kwargs):
        thresholds.append(invoke_kwargs["sleep_threshold"])
        return await invoke(query, *args, **invoke_kwargs)

    client.invoke = recording_invoke

    call(lambda: client.copy_media_group(-100123, -100123, 1, media_group=album, **kwargs))

    assert thresholds == [expected]


RAW_BASIC_GROUP = raw.types.Chat(
    id=45, title="Group", photo=raw.types.ChatPhotoEmpty(), participants_count=1, date=1, version=1
)


def album_reader(groups, omit_missing=True, peer=raw.types.PeerChannel(channel_id=123)):
    """A client whose server knows the messages of `groups`, a dict of id: media group, and answers in reverse order."""
    def respond(query):
        client.requested_ids = [ref.id for ref in query.id]
        messages = []

        for ref in reversed(query.id):
            if ref.id in groups:
                messages.append(raw.types.Message(
                    id=ref.id, peer_id=peer, date=1, message="",
                    grouped_id=groups[ref.id], media=raw.types.MessageMediaPhoto(photo=raw_photo(3000 + ref.id))
                ))
            elif not omit_missing:
                messages.append(raw.types.MessageEmpty(id=ref.id))

        return raw.types.messages.Messages(
            messages=messages, topics=[], users=[RAW_USER], chats=[RAW_CHANNEL, RAW_BASIC_GROUP]
        )

    client = offline_client(PEERS, respond)
    return client


@pytest.mark.parametrize("omit_missing", [True, False])
def test_get_media_group_finds_the_message_by_id_and_sorts_the_result(omit_missing):
    client = album_reader({20: 1, 21: 1, 22: 1, 23: 2}, omit_missing)

    album = call(lambda: client.get_media_group(-100123, 21))

    assert [message.id for message in album] == [20, 21, 22]


@pytest.mark.parametrize("message_id", [21, 30])
def test_get_media_group_of_a_message_without_an_album(message_id):
    client = album_reader({21: None, 22: 1})

    with pytest.raises(ValueError):
        call(lambda: client.get_media_group(-100123, message_id))


@pytest.mark.parametrize("chat_id, peer, message_id, expected", [
    (-100123, raw.types.PeerChannel(channel_id=123), 21, range(12, 31)),
    (-100123, raw.types.PeerChannel(channel_id=123), 3, range(1, 13)),
    (7, raw.types.PeerUser(user_id=7), 100, range(51, 150)),
    (-45, raw.types.PeerChat(chat_id=45), 100, range(51, 150)),
], ids=["channel", "first messages", "user", "basic group"])
def test_get_media_group_reads_more_ids_where_they_are_shared(chat_id, peer, message_id, expected):
    client = album_reader({message_id: 1}, peer=peer)

    call(lambda: client.get_media_group(chat_id, message_id))

    assert client.requested_ids == list(expected)


def test_get_media_group_with_other_messages_in_between():
    album_ids = [100, 101, 102, 103, 104, 105, 106, 107, 108, 111]
    client = album_reader({i: 1 for i in album_ids}, peer=raw.types.PeerUser(user_id=7))

    album = call(lambda: client.get_media_group(7, 100))

    assert [message.id for message in album] == album_ids


def test_get_media_group_only_parses_the_media_group():
    def respond(query):
        return raw.types.messages.Messages(messages=[
            # A message of another chat, which would need another request to be parsed
            raw.types.MessageService(
                id=20, peer_id=raw.types.PeerUser(user_id=7), date=1, action=raw.types.MessageActionPinMessage(),
                reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=5)
            ),
            raw.types.Message(
                id=21, peer_id=raw.types.PeerUser(user_id=7), date=1, message="", grouped_id=1,
                media=raw.types.MessageMediaPhoto(photo=raw_photo())
            ),
        ], topics=[], users=[RAW_USER], chats=[])

    client = offline_client(PEERS, respond)

    album = call(lambda: client.get_media_group(7, 21))

    assert [message.id for message in album] == [21]
    assert len(client.sent) == 1


def test_copy_keeps_the_settings_of_polls():
    bold = raw.types.MessageEntityBold(offset=0, length=4)
    media = raw_poll(
        "q", ("one", None), ("two", None), quiz=True, multiple_choice=True, public_voters=True, shuffle_answers=True,
        open_answers=True, revoting_disabled=True,
        results=raw.types.PollResults(
            results=[raw.types.PollAnswerVoters(option=bytes([1]), voters=1, correct=True)],
            solution="**because**", solution_entities=[bold]
        )
    )

    request = copy_media(media, "<b>description</b>")

    poll = request.media.poll
    assert poll.quiz and poll.multiple_choice and poll.public_voters
    assert poll.shuffle_answers and poll.open_answers and poll.revoting_disabled
    assert request.media.correct_answers == [1]
    assert request.media.solution == "**because**" and written(request.media.solution_entities) == [bold.write()]
    assert request.message == "<b>description</b>" and not request.entities


def test_copy_of_a_poll_with_a_new_description():
    request = copy_media(raw_poll("q", ("one", None), ("two", None)), "original", caption="<b>new</b>", parse_mode=enums.ParseMode.HTML)

    assert request.message == "new" and written(request.entities) == [raw.types.MessageEntityBold(offset=0, length=3).write()]


def test_copy_of_a_quiz_without_the_correct_options_is_a_regular_poll():
    request = copy_media(raw_poll("q", ("one", None), ("two", None), quiz=True))

    assert not request.media.poll.quiz and not request.media.correct_answers


def test_copy_of_a_poll_with_public_voters_to_a_channel_is_anonymous():
    attempts = []

    def respond(query):
        attempts.append((query.media.poll.public_voters, query.media.poll.open_answers))

        if query.media.poll.public_voters:
            raise BroadcastPublicVotersForbidden

        return empty_updates()

    client = offline_client(PEERS, respond)
    media = raw_poll("q", ("one", None), ("two", None), public_voters=True, open_answers=True)
    message = asyncio.run(parse_message(client, 1, media, "", None, False))

    call(lambda: message.copy(-100123))

    assert [(bool(public), bool(options)) for public, options in attempts] == [(True, True), (False, False)]


@pytest.mark.parametrize("user_id, kept", [(7, True), (8, False)], ids=["known user", "missing user"])
def test_copy_of_a_quiz_leaves_out_the_mentions_it_cannot_reach(user_id, kept):
    mention = raw.types.MessageEntityMentionName(offset=0, length=1, user_id=user_id)
    media = raw_poll("q", ("one", None), ("two", None), quiz=True, results=raw.types.PollResults(
        results=[raw.types.PollAnswerVoters(option=bytes([0]), voters=1, correct=True)],
        solution="s", solution_entities=[mention]
    ))

    request = copy_media(media)

    assert request.media.solution == "s" and bool(request.media.solution_entities) == kept


@pytest.mark.parametrize("kwargs", [{"service": enums.MessageServiceType.NEW_CHAT_TITLE}, {"empty": True}], ids=["service", "empty"])
def test_copy_of_what_cant_be_copied_needs_no_client(kwargs):
    chat = types.Chat(id=1, type=enums.ChatType.PRIVATE)
    message = pickle.loads(pickle.dumps(types.Message(id=1, chat=chat, **kwargs)))

    assert call(lambda: message.copy(1)) is None


def test_copy_of_a_text_passes_the_quote_through_parse_mode():
    request = copy_media(None, "<b>original</b>", reply_to_message_id=3, quote_text="<b>q</b>", parse_mode=enums.ParseMode.HTML)

    assert request.message == "<b>original</b>" and not request.entities
    assert request.reply_to.quote_text == "q"
    assert written(request.reply_to.quote_entities) == [raw.types.MessageEntityBold(offset=0, length=1).write()]


def test_get_messages_only_finds_the_messages_of_the_chat():
    client = album_reader({21: None}, peer=raw.types.PeerUser(user_id=7))

    async def user_id():
        return 7

    client.storage.user_id = user_id

    assert not call(lambda: client.get_messages(7, 21)).empty
    assert not call(lambda: client.get_messages("me", 21)).empty
    assert call(lambda: client.get_messages(-45, 21)).empty
    assert [message.empty for message in call(lambda: client.get_messages(-45, [21]))] == [True]


def test_get_messages_finds_replies_in_other_chats():
    client = album_reader({21: None}, peer=raw.types.PeerUser(user_id=7))

    assert not call(lambda: client.get_messages(-45, reply_to_message_ids=21, replies=0)).empty


def test_media_group_of_another_chat_cannot_be_read_or_copied():
    client = album_reader({21: 1, 22: 1}, peer=raw.types.PeerUser(user_id=7))

    for read in (
        lambda: client.get_media_group(-45, 21),
        lambda: client.copy_media_group(-100123, -45, 21),
        lambda: client.forward_media_group(-100123, -45, 21),
    ):
        with pytest.raises(ValueError):
            call(read)

    assert not [q for q in client.sent if isinstance(q, (raw.functions.messages.SendMultiMedia, raw.functions.messages.ForwardMessages))]


def test_copy_message_of_another_chat_sends_nothing():
    client = album_reader({21: None}, peer=raw.types.PeerUser(user_id=7))

    assert call(lambda: client.copy_message(-100123, -45, 21)) is None
    assert [type(q) for q in client.sent] == [raw.functions.messages.GetMessages]


RAW_FORUM_TOPIC = raw.types.ForumTopic(
    id=7, date=1, peer=raw.types.PeerChannel(channel_id=123), title="x", icon_color=1, top_message=1,
    read_inbox_max_id=0, read_outbox_max_id=0, unread_count=0, unread_mentions_count=0, unread_reactions_count=0,
    unread_poll_votes_count=0, from_id=raw.types.PeerChannel(channel_id=123),
    notify_settings=raw.types.PeerNotifySettings()
)
IN_TOPIC = raw.types.MessageReplyHeader(reply_to_msg_id=7, forum_topic=True)
RAW_FORUM = raw.types.Channel(
    id=123, access_hash=1, title="Test", photo=raw.types.ChatPhotoEmpty(), date=0, megagroup=True, forum=True
)
# The id of RAW_CHANNEL after parsing it, which is what parsed messages use to read again
PARSED_CHANNEL_PEERS = {**PEERS, utils.get_channel_id(123): PEERS[-100123]}


def forum_client(topics=(RAW_FORUM_TOPIC,)):
    """A client whose server answers like a forum: reads have the topics, and what is sent goes to the topic."""
    def respond(query):
        if isinstance(query, raw.functions.channels.GetMessages):
            return raw.types.messages.ChannelMessages(
                pts=1, count=len(query.id), topics=list(topics), chats=[RAW_FORUM], users=[],
                messages=[
                    raw.types.Message(id=ref.id, peer_id=raw.types.PeerChannel(channel_id=123), date=1, message="", reply_to=IN_TOPIC)
                    for ref in query.id
                ]
            )

        return sent_photos(len(getattr(query, "multi_media", [None])), IN_TOPIC, RAW_FORUM)

    return offline_client(PARSED_CHANNEL_PEERS, respond)


def reads(client):
    return len([q for q in client.sent if isinstance(q, raw.functions.channels.GetMessages)])


def topic_message_from_an_update(client):
    """Parse a message of the topic as the dispatcher does with an update."""
    update = roundtrip(raw.types.UpdateNewChannelMessage(
        message=raw.types.Message(id=1, peer_id=raw.types.PeerChannel(channel_id=123), date=1, message="", reply_to=IN_TOPIC),
        pts=1, pts_count=1
    ))
    message, _ = call(lambda: client.dispatcher.update_parsers[type(update)](update, {}, {123: roundtrip(RAW_FORUM)}))
    return message


def assert_sent_to_the_topic_without_reading(client, message):
    [request] = client.sent
    assert not isinstance(request, (raw.functions.channels.GetMessages, raw.functions.messages.GetMessages))
    assert (request.top_msg_id if isinstance(request, raw.functions.messages.ForwardMessages)
            else request.reply_to.top_msg_id) == 7
    assert message.message_thread_id == 7 and message.is_topic_message and message.topic is None


SENT_TO_A_TOPIC = {
    "send_message": lambda client: client.send_message(-100123, "text", message_thread_id=7),
    "send_cached_media": lambda client: client.send_cached_media(-100123, photo_file_id(2001), message_thread_id=7),
    "send_contact": lambda client: client.send_contact(-100123, "1", "a", message_thread_id=7),
    "send_location": lambda client: client.send_location(-100123, 1.0, 2.0, message_thread_id=7),
    "send_venue": lambda client: client.send_venue(-100123, 1.0, 2.0, "t", "a", message_thread_id=7),
    "send_poll": lambda client: client.send_poll(
        -100123, "q", [types.PollOption(text="one"), types.PollOption(text="two")], message_thread_id=7
    ),
    "send_game": lambda client: client.send_game(-100123, "game", message_thread_id=7),
    "send_web_page": lambda client: client.send_web_page(-100123, "https://example.com", message_thread_id=7),
    "forward_messages": lambda client: client.forward_messages(-100123, 7, 1, message_thread_id=7),
    "send_photo": lambda client: client.send_photo(-100123, photo_file_id(2001), message_thread_id=7),
    "send_dice": lambda client: client.send_dice(-100123, message_thread_id=7),
    **{
        method: (lambda client, method=method, file_type=file_type: getattr(client, method)(
            -100123, document_file_id(file_type, 1), message_thread_id=7
        )) for method, file_type in {
            "send_document": FileType.DOCUMENT, "send_video": FileType.VIDEO, "send_audio": FileType.AUDIO,
            "send_animation": FileType.ANIMATION, "send_voice": FileType.VOICE, "send_video_note": FileType.VIDEO_NOTE,
            "send_sticker": FileType.STICKER
        }.items()
    },
}
COPIED_TO_A_TOPIC = {
    "text": (None, "text"),
    "photo": (raw.types.MessageMediaPhoto(photo=raw_photo()), "caption"),
    **{name: (media, "") for name, media in MEDIA_WITHOUT_CAPTION.items()},
    "game": (raw.types.MessageMediaGame(game=raw.types.Game(
        id=1, access_hash=2, short_name="game", title="t", description="d", photo=raw_photo()
    )), ""),
    "web page": (raw.types.MessageMediaWebPage(webpage=raw.types.WebPage(
        id=1, url="https://example.com", display_url="example.com", hash=0
    )), ""),
}


@pytest.mark.parametrize("send", SENT_TO_A_TOPIC.values(), ids=SENT_TO_A_TOPIC.keys())
def test_sending_to_a_topic_does_not_read_the_topic(send):
    client = forum_client()

    message = call(lambda: send(client))

    assert_sent_to_the_topic_without_reading(client, message)


def test_editing_in_a_topic_does_not_read_the_topic():
    def respond(query):
        return empty_updates(updates=[raw.types.UpdateEditChannelMessage(
            message=raw.types.Message(id=1, peer_id=raw.types.PeerChannel(channel_id=123), date=1, message="e", reply_to=IN_TOPIC),
            pts=1, pts_count=1
        )], chats=[RAW_FORUM])

    client = offline_client(PARSED_CHANNEL_PEERS, respond)

    message = call(lambda: client.edit_message_text(-100123, 1, "e"))

    assert reads(client) == 0 and message.message_thread_id == 7 and message.topic is None


@pytest.mark.parametrize("media, text", COPIED_TO_A_TOPIC.values(), ids=COPIED_TO_A_TOPIC.keys())
def test_copying_to_a_topic_does_not_read_the_topic(media, text):
    client = forum_client()

    async def is_bot(value=None):
        return True

    client.storage.is_bot = is_bot
    original = asyncio.run(parse_message(client, 1, media, text, None, False))

    message = call(lambda: original.copy(-100123, message_thread_id=7))

    assert_sent_to_the_topic_without_reading(client, message)


@pytest.mark.parametrize("media, text", [COPIED_TO_A_TOPIC["text"], COPIED_TO_A_TOPIC["photo"]], ids=["text", "photo"])
def test_forwarding_to_a_topic_does_not_read_the_topic(media, text):
    client = forum_client()
    original = asyncio.run(parse_message(client, 1, media, text, None, False))

    message = call(lambda: original.forward(-100123, message_thread_id=7))

    assert_sent_to_the_topic_without_reading(client, message)


@pytest.mark.parametrize("send", [
    lambda client, original: client.send_message(-100123, "text", reply_to_message_id=9),
    lambda client, original: original.copy(-100123, reply_to_message_id=9),
], ids=["send_message", "Message.copy"])
@pytest.mark.parametrize("reply_to, thread_id", [
    (raw.types.MessageReplyHeader(reply_to_msg_id=9, reply_to_top_id=7, forum_topic=True), 7),
    (raw.types.MessageReplyHeader(reply_to_msg_id=9), 1),
], ids=["in a topic", "in the general topic"])
def test_a_reply_still_reads_the_replied_message(send, reply_to, thread_id):
    def respond(query):
        if isinstance(query, raw.functions.channels.GetMessages):
            return raw.types.messages.ChannelMessages(
                pts=1, count=1, topics=[], chats=[RAW_FORUM], users=[],
                messages=[raw.types.Message(id=9, peer_id=raw.types.PeerChannel(channel_id=123), date=1, message="replied")]
            )

        return sent_photos(1, reply_to, RAW_FORUM)

    client = offline_client(PARSED_CHANNEL_PEERS, respond)
    original = asyncio.run(parse_message(client, 1, None, "text", None, False))

    message = call(lambda: send(client, original))

    assert reads(client) == 1 and message.reply_to_message_id == 9 and message.reply_to_message.text == "replied"
    assert message.message_thread_id == thread_id and message.is_topic_message and message.topic is None


def test_a_message_of_a_topic_from_an_update_still_reads_the_topic():
    client = forum_client()

    message = topic_message_from_an_update(client)

    assert reads(client) == 1 and message.message_thread_id == 7 and message.topic.id == 7


def test_send_media_group_to_a_topic_does_not_read():
    client = forum_client()
    photo = types.InputMediaPhoto(photo_file_id(2001))

    messages = call(lambda: client.send_media_group(-100123, [photo, photo], message_thread_id=7, reply_to_message_id=9))

    assert len(client.sent) == 1 and isinstance(client.sent[0], raw.functions.messages.SendMultiMedia)
    assert [(message.message_thread_id, message.topic) for message in messages] == [(7, None), (7, None)]


def test_send_paid_media_does_not_read():
    client = forum_client()

    messages = call(lambda: client.send_paid_media(-100123, 1, [types.InputMediaPhoto(photo_file_id(2001))]))

    assert len(client.sent) == 1 and messages[0].message_thread_id == 7 and messages[0].topic is None


def test_copy_message_does_not_read_the_replied_message():
    def respond(query):
        if isinstance(query, raw.functions.channels.GetMessages):
            return raw.types.messages.ChannelMessages(
                pts=1, count=1, topics=[], chats=[RAW_CHANNEL], users=[],
                messages=[raw.types.Message(
                    id=1, peer_id=raw.types.PeerChannel(channel_id=123), date=1, message="text",
                    reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=3)
                )]
            )

        return empty_updates()

    client = offline_client(PARSED_CHANNEL_PEERS, respond)

    call(lambda: client.copy_message(-100123, -100123, 1))

    assert reads(client) == 1


@pytest.mark.parametrize("topics", [[], [raw.types.ForumTopicDeleted(id=99)]], ids=["none", "another topic"])
def test_a_read_without_the_topic_is_not_repeated(topics):
    client = forum_client(topics)

    message = topic_message_from_an_update(client)

    assert reads(client) == 1 and message.message_thread_id == 7 and message.topic is None


def test_a_response_without_the_topic_is_not_read_again():
    client = forum_client()
    response = raw.types.messages.ChannelMessages(
        pts=1, count=1, topics=[raw.types.ForumTopicDeleted(id=99)], chats=[RAW_CHANNEL], users=[],
        messages=[raw.types.Message(id=1, peer_id=raw.types.PeerChannel(channel_id=123), date=1, message="", reply_to=IN_TOPIC)]
    )

    [message] = call(lambda: utils.parse_messages(client, roundtrip(response), replies=0))

    assert reads(client) == 0 and message.message_thread_id == 7 and message.topic is None


def test_send_game_returns_a_scheduled_message():
    client = offline_client(PEERS, lambda q: empty_updates(updates=[raw.types.UpdateNewScheduledMessage(
        message=raw.types.Message(id=1, peer_id=raw.types.PeerUser(user_id=7), date=1, message="")
    )], users=[RAW_USER]))

    message = call(lambda: client.send_game(7, "game", schedule_date=datetime(2030, 1, 1, tzinfo=timezone.utc)))

    assert message.id == 1 and message.scheduled
