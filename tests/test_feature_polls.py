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

import asyncio
import os
import tempfile

import pytest

import pyrogram
from pyrogram import enums, raw, types, utils
from tests.test_layer_roundtrip import (
    DOCUMENT_FILE_ID, PEERS, PHOTO_FILE_ID, RAW_CHANNEL, RAW_USER, call, empty_updates, offline_client, parse_message,
    roundtrip
)

RAW_PHOTO = raw.types.Photo(id=11, access_hash=12, file_reference=b"r", date=1, dc_id=2, sizes=[
    raw.types.PhotoSize(type="x", w=10, h=10, size=100)
])


def text(value):
    return raw.types.TextWithEntities(text=value, entities=[])


def test_send_poll_with_layer_230_options():
    client = offline_client(PEERS, lambda q: empty_updates())
    call(lambda: client.send_poll(
        -100123, "Q?",
        [types.PollOption("a"), types.PollOption("b", media=types.InputMediaDocument(DOCUMENT_FILE_ID)),
         types.PollOption("c")],
        type=enums.PollType.QUIZ, correct_option_ids=[0, 2], explanation="because",
        description="**Pick** one", media=types.InputMediaPhoto(PHOTO_FILE_ID),
        explanation_media=types.InputMediaPhoto("https://telegram.org/img/t_logo.png"),
        allows_revoting=False, allow_adding_options=True, shuffle_options=True, hide_results_until_closes=True,
        members_only=True, country_codes=["ID", "US"]
    ))

    request = client.sent[-1]
    assert request.message == "Pick one" and isinstance(request.entities[0], raw.types.MessageEntityBold)
    media = request.media
    poll = media.poll
    assert poll.revoting_disabled and poll.open_answers and poll.shuffle_answers and poll.hide_results_until_close
    assert poll.subscribers_only and poll.countries_iso2 == ["ID", "US"] and poll.quiz
    assert media.correct_answers == [0, 2] and media.solution == "because"
    assert isinstance(media.attached_media, raw.types.InputMediaPhoto)
    assert isinstance(media.solution_media, raw.types.InputMediaPhotoExternal)
    assert [type(a.media) for a in poll.answers] == [type(None), raw.types.InputMediaDocument, type(None)]


def test_send_poll_sends_the_solution_only_for_quizzes():
    client = offline_client(PEERS, lambda q: empty_updates())
    call(lambda: client.send_poll(
        -100123, "Q?", [types.PollOption("a"), types.PollOption("b")], correct_option_id=1,
        explanation="because", explanation_media=types.InputMediaPhoto(PHOTO_FILE_ID)
    ))

    media = client.sent[-1].media
    assert not media.poll.quiz and not media.correct_answers
    assert media.solution is None and not media.solution_entities and media.solution_media is None


def test_send_quiz_with_empty_explanation():
    client = offline_client(PEERS, lambda q: empty_updates())
    call(lambda: client.send_poll(
        -100123, "Q?", [types.PollOption("a"), types.PollOption("b")], type=enums.PollType.QUIZ,
        correct_option_id=1, explanation="<b></b>", explanation_media=types.InputMediaPhoto(PHOTO_FILE_ID)
    ))

    media = client.sent[-1].media
    assert media.poll.quiz and media.correct_answers == [1]
    assert media.solution is None and not media.solution_entities
    assert isinstance(media.solution_media, raw.types.InputMediaPhoto)


def test_send_poll_explanation_entities_need_an_explanation():
    client = offline_client(PEERS, lambda q: empty_updates())
    bold = types.MessageEntity(type=enums.MessageEntityType.BOLD, offset=0, length=1)

    with pytest.raises(ValueError):
        call(lambda: client.send_poll(
            -100123, "Q?", [types.PollOption("a"), types.PollOption("b")], type=enums.PollType.QUIZ,
            correct_option_id=0, explanation_entities=[bold]
        ))
    assert client.sent == []


def test_send_poll_uploads_local_media(monkeypatch):
    def respond(query):
        if isinstance(query, raw.functions.messages.UploadMedia):
            assert isinstance(query.media, raw.types.InputMediaUploadedPhoto)
            return raw.types.MessageMediaPhoto(photo=RAW_PHOTO)
        return empty_updates()

    client = offline_client(PEERS, respond)

    async def save_file(path, *args, **kwargs):
        return raw.types.InputFile(id=1, parts=1, name=os.path.basename(path), md5_checksum="")

    client.save_file = save_file

    with tempfile.NamedTemporaryFile(suffix=".jpg") as f:
        call(lambda: client.send_poll(-100123, "Q?", [types.PollOption("a"), types.PollOption("b")],
                                      media=types.InputMediaPhoto(f.name)))

    attached = client.sent[-1].media.attached_media
    assert isinstance(attached, raw.types.InputMediaPhoto) and attached.id.id == 11


def test_poll_parses_layer_230_fields():
    raw_poll = raw.types.Poll(
        id=1, hash=7, quiz=True, revoting_disabled=True, open_answers=True, shuffle_answers=True,
        hide_results_until_close=True, subscribers_only=True, creator=True, countries_iso2=["ID"],
        question=text("Q?"),
        answers=[
            raw.types.PollAnswer(text=text("a"), option=b"A", media=raw.types.MessageMediaPhoto(photo=RAW_PHOTO)),
            raw.types.PollAnswer(text=text("b"), option=b"B", added_by=raw.types.PeerUser(user_id=7), date=1700000000),
        ]
    )
    results = raw.types.PollResults(
        has_unread_votes=True, can_view_stats=True,
        results=[raw.types.PollAnswerVoters(option=b"A", correct=True, voters=1, recent_voters=[]),
                 raw.types.PollAnswerVoters(option=b"B", correct=True, voters=2, recent_voters=[])],
        solution="why", solution_entities=[], solution_media=raw.types.MessageMediaGeo(
            geo=raw.types.GeoPoint(long=1.5, lat=2.5, access_hash=0))
    )
    media = roundtrip(raw.types.MessageMediaPoll(
        poll=raw_poll, results=results,
        attached_media=raw.types.MessageMediaGeo(geo=raw.types.GeoPoint(long=3.5, lat=4.5, access_hash=0))
    ))
    poll = asyncio.run(types.Poll._parse(None, media, {7: roundtrip(RAW_USER)}, {}))

    assert poll.allows_revoting is False and poll.allows_adding_options and poll.shuffle_options
    assert poll.hide_results_until_closes and poll.members_only and poll.country_codes == ["ID"]
    assert poll.is_creator and poll.has_unread_votes and poll.can_view_statistics
    assert poll.correct_option_ids == [0, 1] and poll.correct_option_id == 0
    assert isinstance(poll.media, types.Location) and poll.media.latitude == 4.5
    assert isinstance(poll.explanation_media, types.Location) and poll.explanation_media.longitude == 1.5
    a, b = poll.options
    assert isinstance(a.media, types.Photo) and a.author is None
    assert b.author.id == 7 and b.addition_date.timestamp() == 1700000000


def test_poll_update_with_only_results():
    # Results need not follow the order of the poll options: the ids refer to the options built from them
    update = roundtrip(raw.types.UpdateMessagePoll(poll_id=1, results=raw.types.PollResults(
        total_voters=3,
        results=[raw.types.PollAnswerVoters(option=b"B", correct=True, voters=1, recent_voters=[]),
                 raw.types.PollAnswerVoters(option=b"A", chosen=True, correct=True, voters=2, recent_voters=[])]
    )))
    poll = asyncio.run(types.Poll._parse_update(None, update, {}))

    assert [(o.data, o.voter_count) for o in poll.options] == [(b"B", 1), (b"A", 2)]
    assert poll.chosen_option_id == 1 and poll.options[poll.chosen_option_id].data == b"A"
    assert poll.correct_option_ids == [0, 1] and poll.correct_option_id == 0


def test_poll_option_author_by_peer_type():
    # A channel and a user with the same raw identifier
    channel = raw.types.Channel(id=7, access_hash=1, title="Channel", photo=raw.types.ChatPhotoEmpty(), date=0)
    users, chats = {7: roundtrip(RAW_USER)}, {7: roundtrip(channel)}

    for peer, expected in ((raw.types.PeerChannel(channel_id=7), -1000000000007), (raw.types.PeerUser(user_id=7), 7)):
        answer = roundtrip(raw.types.PollAnswer(text=text("c"), option=b"C", added_by=peer, date=1))
        option = asyncio.run(types.PollOption._parse(None, answer, users, chats))
        assert option.author.id == expected

    unknown = raw.types.PeerChat(chat_id=8)
    answer = roundtrip(raw.types.PollAnswer(text=text("c"), option=b"C", added_by=unknown, date=1))
    assert asyncio.run(types.PollOption._parse(None, answer, users, chats)).author is None


def external_reply(media, chats):
    reply = roundtrip(raw.types.MessageReplyHeader(
        reply_to_msg_id=9, reply_to_peer_id=raw.types.PeerChannel(channel_id=123),
        reply_from=raw.types.MessageFwdHeader(date=1, from_name="Somebody"), reply_media=media
    ))
    return asyncio.run(types.ExternalReplyInfo._parse(None, reply, {}, chats))


def test_external_reply_to_poll():
    info = external_reply(raw.types.MessageMediaPoll(
        poll=raw.types.Poll(id=1, hash=0, question=text("Q?"), answers=[
            raw.types.PollAnswer(text=text("a"), option=b"A"),
            raw.types.PollAnswer(text=text("b"), option=b"B", added_by=raw.types.PeerChannel(channel_id=123), date=1),
        ]),
        results=raw.types.PollResults(total_voters=1, results=[
            raw.types.PollAnswerVoters(option=b"A", chosen=True, voters=1, recent_voters=[])
        ])
    ), {123: roundtrip(RAW_CHANNEL)})

    assert info.media == enums.MessageMediaType.POLL and isinstance(info.poll, types.Poll)  # used to be a coroutine
    assert [o.text for o in info.poll.options] == ["a", "b"] and info.poll.chosen_option_id == 0
    assert info.poll.options[1].author.id == utils.get_channel_id(123)


def test_external_reply_to_giveaway():
    info = external_reply(raw.types.MessageMediaGiveaway(
        channels=[123], quantity=3, stars=500, until_date=1700000000
    ), {123: roundtrip(RAW_CHANNEL)})

    assert info.media == enums.MessageMediaType.GIVEAWAY and isinstance(info.giveaway, types.Giveaway)
    assert info.giveaway.quantity == 3 and info.giveaway.stars == 500
    assert info.giveaway.chats[0].id == utils.get_channel_id(123)


def test_poll_option_service_messages():
    for action, service, attribute in (
        (raw.types.MessageActionPollAppendAnswer, enums.MessageServiceType.POLL_OPTION_ADDED, "poll_option_added"),
        (raw.types.MessageActionPollDeleteAnswer, enums.MessageServiceType.POLL_OPTION_DELETED, "poll_option_deleted"),
    ):
        message = parse_message(raw.types.MessageService(
            id=2, peer_id=raw.types.PeerUser(user_id=7), from_id=raw.types.PeerUser(user_id=7), date=1,
            reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=5),
            action=action(answer=raw.types.PollAnswer(
                text=text("new"), option=b"N", added_by=raw.types.PeerUser(user_id=7), date=1))
        ), users=[RAW_USER])

        assert message.service == service and message.reply_to_message_id == 5
        option = getattr(message, attribute)
        assert option.text == "new" and option.data == b"N" and option.author.id == 7


def test_add_and_delete_poll_option():
    client = offline_client(PEERS, lambda q: empty_updates())
    assert call(lambda: client.add_poll_option(
        -100123, 5, "**new**", media=types.InputMediaDocument(DOCUMENT_FILE_ID))) is True
    added = client.sent[-1]
    assert isinstance(added, raw.functions.messages.AddPollAnswer) and added.msg_id == 5
    assert isinstance(added.answer, raw.types.InputPollAnswer) and added.answer.text.text == "new"
    assert isinstance(added.answer.media, raw.types.InputMediaDocument)

    assert call(lambda: client.delete_poll_option(-100123, 5, types.PollOption("x", data=b"X"))) is True
    deleted = client.sent[-1]
    assert isinstance(deleted, raw.functions.messages.DeletePollAnswer) and deleted.option == b"X"


def test_unread_poll_votes():
    pages = iter([
        raw.types.messages.Messages(messages=[
            raw.types.Message(id=i, peer_id=raw.types.PeerUser(user_id=7), date=1, message="m") for i in (9, 8)
        ], topics=[], chats=[], users=[RAW_USER]),
        raw.types.messages.Messages(messages=[], topics=[], chats=[], users=[]),
    ])
    client = offline_client(PEERS, lambda q: next(pages))

    async def collect():
        return [m.id async for m in client.get_unread_poll_votes(7)]

    assert asyncio.run(collect()) == [9, 8]
    assert [q.offset_id for q in client.sent] == [0, 8]

    affected = iter([raw.types.messages.AffectedHistory(pts=1, pts_count=1, offset=3),
                     raw.types.messages.AffectedHistory(pts=2, pts_count=1, offset=0)])
    client = offline_client(PEERS, lambda q: next(affected))
    assert call(lambda: client.read_all_chat_poll_votes(-100123, message_thread_id=4)) is True
    assert len(client.sent) == 2 and all(q.top_msg_id == 4 for q in client.sent)


def test_unread_poll_votes_counters():
    dialog = roundtrip(raw.types.Dialog(
        peer=raw.types.PeerUser(user_id=7), top_message=1, read_inbox_max_id=1, read_outbox_max_id=1,
        unread_count=0, unread_mentions_count=0, unread_reactions_count=0, unread_poll_votes_count=4,
        notify_settings=raw.types.PeerNotifySettings()
    ))
    parsed = types.Dialog._parse(None, dialog, {}, {7: roundtrip(RAW_USER)}, {})
    assert parsed.unread_poll_votes_count == 4
    assert pyrogram.enums.MessagesFilter.POLL.value is raw.types.InputMessagesFilterPoll
