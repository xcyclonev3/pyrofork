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
import os
from types import SimpleNamespace

import pytest

from pyrogram import enums, raw, types, utils
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


@pytest.mark.xfail(strict=True, reason="Message.photo gets the live video once message.py passes "
                                       "live_video=media.video to Photo._parse (main session)")
def test_message_with_live_photo():
    from tests.test_layer_roundtrip import parse_message

    message = parse_message(raw.types.Message(
        id=1, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7),
        date=1, message="", media=live_photo_media()
    ), users=[RAW_USER])
    assert message.photo is not None
    assert_live_video(message.photo.live_video)


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
